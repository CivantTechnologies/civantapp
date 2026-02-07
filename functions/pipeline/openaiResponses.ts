export async function callStructuredAgent<T = unknown>(params: {
  model?: string;
  systemPrompt: string;
  userPrompt: string;
  schemaName: string;
  schema: Record<string, unknown>;
}): Promise<T> {
  const apiKey = Deno.env.get('OPENAI_API_KEY');
  if (!apiKey) {
    throw new Error('OPENAI_API_KEY is required for agentic pipeline');
  }

  const model = params.model || Deno.env.get('OPENAI_MODEL') || 'gpt-4.1-mini';

  const response = await fetch('https://api.openai.com/v1/responses', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${apiKey}`
    },
    body: JSON.stringify({
      model,
      input: [
        { role: 'system', content: [{ type: 'input_text', text: params.systemPrompt }] },
        { role: 'user', content: [{ type: 'input_text', text: params.userPrompt }] }
      ],
      text: {
        format: {
          type: 'json_schema',
          name: params.schemaName,
          schema: params.schema,
          strict: true
        }
      }
    })
  });

  if (!response.ok) {
    const detail = await response.text().catch(() => 'Unknown OpenAI error');
    throw new Error(`OpenAI Responses API failed: ${response.status} ${detail}`);
  }

  const payload = await response.json();

  const outputText = payload?.output_text;
  if (typeof outputText !== 'string' || !outputText.trim()) {
    throw new Error('OpenAI response missing output_text');
  }

  return JSON.parse(outputText) as T;
}
