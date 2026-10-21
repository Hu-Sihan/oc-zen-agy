import { test } from 'node:test';
import assert from 'node:assert/strict';
import { Readable } from 'node:stream';
import { collectCompletion } from '../src/sse.mjs';

const frame = value => `data: ${JSON.stringify(value)}\r\n\r\n`;
const chunk = choices => ({ id: 'chatcmpl-test', object: 'chat.completion.chunk', created: 123, model: 'big-pickle', choices });

test('SSE aggregation handles UTF-8 byte splits, CRLF, reasoning, usage and DONE', async () => {
  const text = ': heartbeat\r\n\r\n' + frame(chunk([{ index: 0, delta: { role: 'assistant', content: '你好🙂', reasoning_content: '想' }, finish_reason: null }]))
    + frame(chunk([{ index: 0, delta: { content: 'OK', reasoning_content: '好' }, finish_reason: 'stop' }]))
    + frame({ ...chunk([]), usage: { prompt_tokens: 10, completion_tokens: 3, total_tokens: 13 } }) + 'data: [DONE]\r\n\r\n';
  const bytes = Buffer.from(text);
  const response = await collectCompletion(Readable.from([...bytes].map(value => Buffer.from([value]))));
  assert.equal(response.object, 'chat.completion');
  assert.equal(response.choices[0].message.content, '你好🙂OK');
  assert.equal(response.choices[0].message.reasoning_content, '想好');
  assert.equal(response.choices[0].finish_reason, 'stop');
  assert.equal(response.usage.total_tokens, 13);
});

test('SSE aggregation merges multiple choices and fragmented function arguments', async () => {
  const text = frame(chunk([{ index: 1, delta: { role: 'assistant', content: 'second' }, finish_reason: 'stop' },
    { index: 0, delta: { role: 'assistant', tool_calls: [{ index: 0, id: 'call_1', type: 'function', function: { name: 'read', arguments: '{"path":' } }] }, finish_reason: null }]))
    + frame(chunk([{ index: 0, delta: { tool_calls: [{ index: 0, function: { arguments: '"file"}' } }] }, finish_reason: 'tool_calls' }])) + 'data: [DONE]\n\n';
  const response = await collectCompletion(Readable.from([Buffer.from(text)]));
  assert.deepEqual(response.choices.map(choice => choice.index), [0, 1]);
  assert.equal(response.choices[0].message.content, null);
  assert.deepEqual(response.choices[0].message.tool_calls[0], { id: 'call_1', type: 'function', function: { name: 'read', arguments: '{"path":"file"}' } });
});

test('SSE errors, incomplete streams and response limits fail explicitly', async () => {
  const source = text => Readable.from([Buffer.from(text)]);
  await assert.rejects(collectCompletion(source(frame({ error: { type: 'FreeTierError', message: 'denied' } }))), { code: 'FreeTierError', status: 502 });
  await assert.rejects(collectCompletion(source(frame(chunk([{ index: 0, delta: { content: 'partial' } }])))), { code: 'incomplete_upstream_stream' });
  await assert.rejects(collectCompletion(source(frame(chunk([]))), { maxBytes: 4 }), { code: 'upstream_response_too_large' });
  await assert.rejects(collectCompletion(source('data: invalid\n\n')), { code: 'invalid_upstream_sse' });
});
