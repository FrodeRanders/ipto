//
//
//

const STATUS_MAP = new Map([
  [1, 'PENDING_DISPOSITION'],
  [10, 'PENDING_DELETION'],
  [20, 'OBLITERATED'],
  [30, 'EFFECTIVE'],
  [40, 'ARCHIVED']
]);

export const snyggifyTime = (rawtime) => {
  if (!rawtime) return 'n/a';
  if (typeof rawtime === 'number') {
    return snyggifyTime(new Date(rawtime).toISOString());
  }
  if (typeof rawtime === 'object') {
    if (typeof rawtime.time === 'number') {
      return snyggifyTime(new Date(rawtime.time).toISOString());
    }
    if (typeof rawtime.epochSecond === 'number') {
      return snyggifyTime(new Date(rawtime.epochSecond * 1000).toISOString());
    }
    return String(rawtime);
  }
  if (typeof rawtime !== 'string') return String(rawtime);
  const pretty = rawtime.replace(/^(\d{4}-\d{2}-\d{2})T(.*?)(?:Z|[+-]\d{2}:\d{2})$/, '$1 $2');
  if (pretty !== rawtime) return pretty;
  const spaced = rawtime.match(/^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})(?:\.(\d+))?Z?$/);
  if (spaced) {
    const fraction = spaced[3];
    if (fraction) {
      const millis = fraction.slice(0, 3).padEnd(3, '0');
      return `${spaced[1]} ${spaced[2]}.${millis}`;
    }
    return `${spaced[1]} ${spaced[2]}`;
  }
  const parsed = new Date(rawtime);
  if (Number.isNaN(parsed.getTime())) return rawtime;
  return snyggifyTime(parsed.toISOString());
};

const mapAttribute = (attr) => {
  // 'attr' refers to ipto:string-scalar et al and it's elements
  if (attr.attrtype === 'RECORD' && Array.isArray(attr.attributes)) {
    return {
      type: attr.attrtype,
      alias: attr.alias,
      name: attr.attrname,
      attributes: attr.attributes.map(mapAttribute)
    };
  }

  if (Array.isArray(attr.value)) {
    const values = attr.value.length === 1 ? attr.value[0] : attr.value;
    if (attr.attrtype === 'TIME') {
      if (Array.isArray(values)) {
        return {
          type: attr.attrtype,
          alias: attr.alias,
          name: attr.attrname,
          value: values.map((value) => snyggifyTime(value))
        };
      }
      return {
        type: attr.attrtype,
        alias: attr.alias,
        name: attr.attrname,
        value: snyggifyTime(values)
      };
    }
    return {
      type: attr.attrtype,
      alias: attr.alias,
      name: attr.attrname,
      value: values
    };
  }

  return {
    type: attr.attrtype,
    alias: attr.alias,
    name: attr.attrname,
    value: null
  };
};

export const mapUnitPayload = (payload) => {
  if (!payload) return null;

  const attributes = {};
  (payload.attributes || []).forEach((attr) => {
    attributes[attr.attrname] = mapAttribute(attr);
  });

  // 'payload' refers to ipto:unit and it's elements
  return {
    id: String(payload.unitid),
    tenantId: payload.tenantid,
    unitId: payload.unitid,
    version: payload.unitver,
    name: payload.unitname || `Unit ${payload.unitid}`,
    status: STATUS_MAP.get(payload.status) || `STATUS_${payload.status}`,
    created: payload.created,
    modified: payload.modified,
    attributes
  };
};

/*
 * /api/trees/roots
 */
export const fetchTreeRoots = async () => {
  const response = await fetch('/api/trees/roots');
  if (!response.ok) {
    throw new Error(`Tree root fetch failed (${response.status})`);
  }
  return response.json();
};

/*
 * /api/trees/${tenantId}/${unitId}/children${suffix}
 */
export const fetchTreeChildren = async (tenantId, unitId, limit = 200) => {
  const params = new URLSearchParams();
  if (limit !== null && limit !== undefined) {
    params.set('limit', limit);
  }
  const suffix = params.toString() ? `?${params.toString()}` : '';
  const response = await fetch(`/api/trees/${tenantId}/${unitId}/children${suffix}`);
  if (!response.ok) {
    throw new Error(`Tree children fetch failed (${response.status})`);
  }
  const payload = await response.json();
  return payload.map(mapUnitPayload);
};

/*
 * /api/tenants
 */
export const fetchTenants = async () => {
  const response = await fetch('/api/tenants');
  if (!response.ok) {
    throw new Error(`Tenant fetch failed (${response.status})`);
  }
  return response.json();
};

/*
 * /api/units/${tenantId}/${unitId}${query}
 */
export const fetchUnitById = async (tenantId, unitId, unitver = null) => {
  const query = unitver === null || unitver === undefined ? '' : `?unitver=${unitver}`;
  const response = await fetch(`/api/units/${tenantId}/${unitId}${query}`);
  if (!response.ok) {
    throw new Error(`Unit fetch failed (${response.status})`);
  }
  const payload = await response.json();
  return mapUnitPayload(payload);
};

/*
 * /api/units${suffix}
 */
export const fetchUnitsList = async (tenantId, limit = 50) => {
  const params = new URLSearchParams();
  if (tenantId !== null && tenantId !== undefined) {
    params.set('tenantId', tenantId);
  }
  if (limit !== null && limit !== undefined) {
    params.set('limit', limit);
  }
  const suffix = params.toString() ? `?${params.toString()}` : '';
  const response = await fetch(`/api/units${suffix}`);
  if (!response.ok) {
    throw new Error(`Units fetch failed (${response.status})`);
  }
  const payload = await response.json();
  return payload.map(mapUnitPayload);
};

/*
 * /api/attributes
 */
export const fetchAttributes = async () => {
  const response = await fetch('/api/attributes');
  if (!response.ok) {
    throw new Error(`Attribute fetch failed (${response.status})`);
  }
  return response.json();
};

/*
 * /api/attributes/metadata
 */
export const fetchAttributeMetadata = async () => {
  const response = await fetch('/api/attributes/metadata');
  if (!response.ok) {
    throw new Error(`Attribute metadata fetch failed (${response.status})`);
  }
  return response.json();
};

/*
 * /api/attributes (create)
 */
export const createAttribute = async ({ alias, attributeName, qualifiedName, type, isArray }) => {
  const response = await fetch('/api/attributes', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({
      alias,
      attributeName,
      qualifiedName,
      type,
      isArray
    })
  });
  if (!response.ok) {
    const payload = await response.json().catch(() => ({}));
    const message = payload.error || `Attribute create failed (${response.status})`;
    throw new Error(message);
  }
  return response.json();
};

/*
 * /api/attributes/{attrId}/descriptions
 */
export const fetchAttributeDescriptions = async (attrId) => {
  const response = await fetch(`/api/attributes/${attrId}/descriptions`);
  if (!response.ok) {
    const payload = await response.json().catch(() => ({}));
    const message = payload.error || `Attribute descriptions fetch failed (${response.status})`;
    throw new Error(message);
  }
  return response.json();
};

export const updateAttributeDescriptions = async (attrId, items) => {
  const response = await fetch(`/api/attributes/${attrId}/descriptions`, {
    method: 'PUT',
    headers: {
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({
      items
    })
  });
  if (!response.ok) {
    const payload = await response.json().catch(() => ({}));
    const message = payload.error || `Attribute descriptions update failed (${response.status})`;
    throw new Error(message);
  }
  return response.json();
};

/*
 * /api/records
 */
export const fetchRecords = async () => {
  const response = await fetch('/api/records');
  if (!response.ok) {
    throw new Error(`Record fetch failed (${response.status})`);
  }
  return response.json();
};

/*
 * /api/records/{recordId}
 */
export const fetchRecordTemplate = async (recordId) => {
  const response = await fetch(`/api/records/${recordId}`);
  if (!response.ok) {
    const payload = await response.json().catch(() => ({}));
    const message = payload.error || `Record template fetch failed (${response.status})`;
    throw new Error(message);
  }
  return response.json();
};

/*
 * /api/records (create)
 */
export const createRecordTemplate = async ({ recordId, name, fields }) => {
  const response = await fetch('/api/records', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({
      recordId,
      name,
      fields
    })
  });
  if (!response.ok) {
    const payload = await response.json().catch(() => ({}));
    const message = payload.error || `Record template create failed (${response.status})`;
    throw new Error(message);
  }
  return response.json();
};

/*
 * /api/records/{recordId} (update)
 */
export const updateRecordTemplate = async ({ recordId, name, fields }) => {
  const response = await fetch(`/api/records/${recordId}`, {
    method: 'PUT',
    headers: {
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({
      name,
      fields
    })
  });
  if (!response.ok) {
    const payload = await response.json().catch(() => ({}));
    const message = payload.error || `Record template update failed (${response.status})`;
    throw new Error(message);
  }
  return response.json();
};

/*
 * /api/templates
 */
export const fetchTemplates = async () => {
  const response = await fetch('/api/templates');
  if (!response.ok) {
    throw new Error(`Template fetch failed (${response.status})`);
  }
  return response.json();
};

/*
 * /api/searches
 */
export const fetchSearches = async () => {
  const response = await fetch('/api/searches');
  if (!response.ok) {
    throw new Error(`Searches fetch failed (${response.status})`);
  }
  return response.json();
};

/*
 * /api/searches/units
 */
export const searchUnits = async ({ tenantId, where, offset = 0, size = 20 }) => {
  const response = await fetch('/api/searches/units', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({
      tenantId,
      where,
      offset,
      size
    })
  });
  if (!response.ok) {
    const payload = await response.json().catch(() => ({}));
    const message = payload.error || `Search failed (${response.status})`;
    throw new Error(message);
  }
  const payload = await response.json();
  return payload.map(mapUnitPayload);
};
