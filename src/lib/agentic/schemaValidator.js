function validateNode(value, schema, path, errors) {
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
        value.forEach((item, index) => validateNode(item, schema.items, `${path}[${index}]`, errors));
      }
      return;
    }

    if (schema.type === 'object') {
      if (!value || typeof value !== 'object' || Array.isArray(value)) {
        errors.push(`${path} must be object`);
        return;
      }
      if (schema.required) {
        schema.required.forEach((key) => {
          if (!(key in value)) {
            errors.push(`${path}.${key} is required`);
          }
        });
      }
      if (schema.properties) {
        Object.entries(schema.properties).forEach(([key, propSchema]) => {
          if (key in value) {
            validateNode(value[key], propSchema, `${path}.${key}`, errors);
          }
        });
      }
      if (schema.additionalProperties === false && schema.properties) {
        Object.keys(value).forEach((key) => {
          if (!(key in schema.properties)) {
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

export function validateAgainstSchema(value, schema) {
  const errors = [];
  validateNode(value, schema, '$', errors);
  return {
    valid: errors.length === 0,
    errors
  };
}
