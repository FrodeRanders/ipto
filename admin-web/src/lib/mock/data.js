export const tenants = [
  {
    id: 1,
    name: 'SCRATCH',
    region: 'North',
    units: 14
  },
  {
    id: 2,
    name: 'ARCHIVE',
    region: 'Central',
    units: 6
  }
];

export const attributes = [
  {
    name: 'dcterms:title',
    type: 'STRING',
    cardinality: 'VECTOR',
    searchable: true
  },
  {
    name: 'dcterms:date',
    type: 'TIME',
    cardinality: 'SCALAR',
    searchable: true
  },
  {
    name: 'ipto:data',
    type: 'DATA',
    cardinality: 'SCALAR',
    searchable: false
  },
  {
    name: 'ipto:record',
    type: 'RECORD',
    cardinality: 'VECTOR',
    searchable: false
  }
];

export const records = [
  {
    name: 'ipto:media',
    fields: ['filename', 'mimetype', 'size', 'duration']
  },
  {
    name: 'ipto:geo',
    fields: ['lat', 'lng', 'precision']
  }
];

export const templates = [
  {
    name: 'Core Metadata',
    description: 'Title, dates, and identifiers',
    attributes: ['dcterms:title', 'dcterms:date']
  },
  {
    name: 'Media Payload',
    description: 'Binary payloads with mime information',
    attributes: ['ipto:data', 'ipto:record']
  }
];

export const unitTree = {
  id: 'root-1',
  name: 'SCRATCH',
  tenantId: 1,
  children: [
    {
      id: 'u-100',
      name: 'Case 100',
      tenantId: 1,
      children: [
        {
          id: 'u-101',
          name: 'Sub-case 101',
          tenantId: 1,
          children: []
        },
        {
          id: 'u-102',
          name: 'Sub-case 102',
          tenantId: 1,
          children: []
        }
      ]
    },
    {
      id: 'u-200',
      name: 'Case 200',
      tenantId: 1,
      children: []
    }
  ]
};

export const units = [
  {
    id: 'u-100',
    tenantId: 1,
    name: 'Case 100',
    status: 'EFFECTIVE',
    created: '2024-08-10T12:01:00Z',
    modified: '2024-08-12T09:31:00Z',
    attributes: {
      'dcterms:title': 'Case 100 - Primary',
      'dcterms:date': '2024-08-10T12:01:00Z',
      'ipto:data': {
        mimetype: 'application/json',
        filename: 'case-100.json',
        value: {
          caseId: 100,
          priority: 'high',
          tags: ['alpha', 'beta']
        }
      }
    }
  },
  {
    id: 'u-101',
    tenantId: 1,
    name: 'Sub-case 101',
    status: 'EFFECTIVE',
    created: '2024-08-11T08:00:00Z',
    modified: '2024-08-12T10:00:00Z',
    attributes: {
      'dcterms:title': 'Sub-case 101',
      'dcterms:date': '2024-08-11T08:00:00Z',
      'ipto:data': {
        mimetype: 'video/mp4',
        filename: 'clip-101.mp4',
        value: null
      }
    }
  },
  {
    id: 'u-200',
    tenantId: 1,
    name: 'Case 200',
    status: 'ARCHIVED',
    created: '2023-07-01T11:30:00Z',
    modified: '2024-02-15T14:15:00Z',
    attributes: {
      'dcterms:title': 'Case 200',
      'dcterms:date': '2023-07-01T11:30:00Z',
      'ipto:record': {
        mimetype: 'application/json',
        filename: 'geo.json',
        value: {
          lat: 59.9139,
          lng: 10.7522,
          precision: 'city'
        }
      }
    }
  }
];

export const searches = [
  {
    id: 's-1',
    name: 'Recent cases',
    template: 'Core Metadata',
    filters: [
      { name: 'dcterms:date', operator: '>=', value: '2024-01-01' }
    ]
  },
  {
    id: 's-2',
    name: 'Media payloads',
    template: 'Media Payload',
    filters: [
      { name: 'ipto:data', operator: 'exists', value: 'true' }
    ]
  }
];
