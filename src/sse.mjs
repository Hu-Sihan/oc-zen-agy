import { StringDecoder } from 'node:string_decoder';

function streamError(code, message) { return Object.assign(new Error(message), { status: 502, code }); }

export async function collectCompletion(stream, { signal, maxBytes = 8 * 1024 * 1024 } = {}) {
  const decoder = new StringDecoder('utf8');
  const result = { object: 'chat.completion' };
  const choices = new Map();
  let buffer = '', data = [], event = '', done = false, bytes = 0;

  function dispatch() {
    if (!data.length) { event = ''; return; }
    const text = data.join('\n');
    data = [];
    if (text.trim() === '[DONE]') { done = true; event = ''; return; }
    let chunk;
    try { chunk = JSON.parse(text); } catch { throw streamError('invalid_upstream_sse', '上游 SSE 包含无效 JSON'); }
    if (chunk.error || event === 'error') throw streamError(chunk.error?.type || 'upstream_stream_error', chunk.error?.message || '上游流返回错误');
    event = '';
    for (const key of ['id', 'created', 'model', 'system_fingerprint', 'service_tier']) if (chunk[key] !== undefined) result[key] = chunk[key];
    if (chunk.usage) result.usage = chunk.usage;
    for (const item of chunk.choices || []) {
      if (!Number.isSafeInteger(item.index) || item.index < 0) throw streamError('invalid_upstream_sse', '上游 choice.index 无效');
      if (!choices.has(item.index)) choices.set(item.index, { index: item.index, message: { role: 'assistant', content: '' }, finish_reason: null });
      const choice = choices.get(item.index);
      const delta = item.delta || {};
      if (delta.role) choice.message.role = delta.role;
      for (const key of ['content', 'refusal', 'reasoning_content', 'reasoning']) if (typeof delta[key] === 'string') choice.message[key] = (choice.message[key] || '') + delta[key];
      for (const call of delta.tool_calls || []) {
        if (!Number.isSafeInteger(call.index) || call.index < 0 || call.index > 1024) throw streamError('invalid_upstream_sse', '上游 tool_call.index 无效');
        choice.message.tool_calls ||= [];
        const target = choice.message.tool_calls[call.index] ||= { type: 'function', function: { name: '', arguments: '' } };
        if (call.id) target.id = (target.id || '') + call.id;
        if (call.type) target.type = call.type;
        if (call.function?.name) target.function.name += call.function.name;
        if (call.function?.arguments) target.function.arguments += call.function.arguments;
      }
      if (delta.function_call) {
        choice.message.function_call ||= { name: '', arguments: '' };
        for (const key of ['name', 'arguments']) if (delta.function_call[key]) choice.message.function_call[key] += delta.function_call[key];
      }
      if (item.finish_reason !== undefined && item.finish_reason !== null) choice.finish_reason = item.finish_reason;
      if (item.logprobs !== undefined) {
        if (item.logprobs === null) choice.logprobs ??= null;
        else {
          choice.logprobs ||= {};
          for (const [key, value] of Object.entries(item.logprobs)) {
            if (Array.isArray(value)) choice.logprobs[key] = [...(choice.logprobs[key] || []), ...value];
            else choice.logprobs[key] = value;
          }
        }
      }
    }
  }
  function line(text) {
    if (!text) return dispatch();
    if (text.startsWith(':')) return;
    const split = text.indexOf(':');
    const key = split === -1 ? text : text.slice(0, split);
    let value = split === -1 ? '' : text.slice(split + 1);
    if (value.startsWith(' ')) value = value.slice(1);
    if (key === 'data') data.push(value);
    if (key === 'event') event = value;
  }
  function parse(final = false) {
    while (true) {
      const match = /\r\n|\n|\r/.exec(buffer);
      if (!match || (!final && match[0] === '\r' && match.index + 1 === buffer.length)) break;
      line(buffer.slice(0, match.index));
      buffer = buffer.slice(match.index + match[0].length);
    }
    if (final) { if (buffer) line(buffer); buffer = ''; dispatch(); }
  }
  const abort = () => stream.destroy(signal.reason);
  signal?.throwIfAborted();
  signal?.addEventListener('abort', abort, { once: true });
  try {
    for await (const chunk of stream) {
      bytes += chunk.length;
      if (bytes > maxBytes) throw streamError('upstream_response_too_large', '上游响应超过合并大小限制');
      buffer += decoder.write(chunk);
      parse();
    }
    buffer += decoder.end();
    parse(true);
  } finally { signal?.removeEventListener('abort', abort); }
  if (!done || !choices.size || !result.id) throw streamError('incomplete_upstream_stream', '上游 SSE 缺少完整响应或结束标记');
  result.choices = [...choices.values()].sort((a, b) => a.index - b.index);
  for (const choice of result.choices) {
    if (!choice.message.content && (choice.message.tool_calls || choice.message.function_call || choice.message.refusal)) choice.message.content = null;
    if (choice.message.tool_calls) choice.message.tool_calls = choice.message.tool_calls.filter(Boolean);
  }
  return result;
}
