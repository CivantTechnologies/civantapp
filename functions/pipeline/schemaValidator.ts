// Minimal strict JSON schema validator for this pipeline's schema subset.
type JSONSchema = {
  type?: string;
  enum?: readonly unknown[];
  required?: readonly string[];
  properties?: Record<string, JSONSchema>;
  items?: JSONSchema;
  additionalProperties?: boolean;
  minItems?: number;
  minimum?: number;
  maximum?: number;
};

type ValidationResult = {
  valid: boolean;
  errors: string[];
};

function validateNode(value: unknown, schema: JSONSchema, path: string, errors: string[]) {
  if (schema.type) {
    if (schema.type === 'array') {
      if (!Array.isArray(value)) {
        errors.push(`${path} must be array`);
        return;
      }
      if (typeof schema.minItems === 'number' && value.length < schema.minItems) {
        errors.push(`${path} must have at least ${schema.minItems} items`);
      }
      if (schema.items) {
        value.forEach((item, index) => validateNode(item, schema.items as JSONSchema, `${path}[${index}]`, errors));
      }
      return;
    }

    if (schema.type === 'object') {
      if (!value || typeof value !== 'object' || Array.isArray(value)) {
        errors.push(`${path} must be object`);
        return;
      }
      const objectValue = value as Record<string, unknown>;
      if (schema.required) {
        schema.required.forEach((key) => {
          if (!(key in objectValue)) {
            errors.push(`${path}.${key} is required`);
          }
        });
      }
      if (schema.properties) {
        for (const [key, propSchema] of Object.entries(schema.properties)) {
          if (key in objectValue) {
            validateNode(objectValue[key], propSchema, `${path}.${key}`, errors);
          }
        }
      }
      if (schema.additionalProperties === false && schema.properties) {
        Object.keys(objectValue).forEach((key) => {
          if (!schema.properties || !(key in schema.properties)) {
            errors.push(`${path}.${key} is not allowed`);
          }
        });
      }
      return;
    }

    if (schema.type === 'string' && typeof value !== 'string') {
      errors.push(`${path} must be string`);
      return;
    }

    if (schema.type === 'number') {
      if (typeof value !== 'number' || Number.isNaN(value)) {
        errors.push(`${path} must be number`);
        return;
      }
      if (typeof schema.minimum === 'number' && value < schema.minimum) {
        errors.push(`${path} must be >= ${schema.minimum}`);
      }
      if (typeof schema.maximum === 'number' && value > schema.maximum) {
        errors.push(`${path} must be <= ${schema.maximum}`);
      }
      return;
    }
  }

  if (schema.enum && !schema.enum.includes(value)) {
    errors.push(`${path} must be one of ${schema.enum.join(', ')}`);
  }
}

export function validateAgainstSchema(value: unknown, schema: JSONSchema): ValidationResult {
  const errors: string[] = [];
  validateNode(value, schema, '$', errors);
  return {
    valid: errors.length === 0,
    errors
  };
}

export function parseAndValidateJson(rawOutput: unknown, schema: JSONSchema): { ok: true; value: unknown } | { ok: false; error: string } {
  let parsed: unknown;

  if (typeof rawOutput === 'string') {
    try {
      parsed = JSON.parse(rawOutput);
    } catch {
      return { ok: false, error: 'Invalid JSON' };
    }
  } else {
    parsed = rawOutput;
  }

  const validation = validateAgainstSchema(parsed, schema);
  if (!validation.valid) {
    return { ok: false, error: `Schema validation failed: ${validation.errors.join('; ')}` };
  }

  return { ok: true, value: parsed };
}
