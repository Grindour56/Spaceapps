#!/usr/bin/env node
// Convert the already-installed world-atlas TopoJSON into local GeoJSON.
const fs = require('node:fs');
const path = require('node:path');
const topojson = require('../app/node_modules/topojson-client');
const topology = require('../app/node_modules/world-atlas/countries-50m.json');
const featureCollection = topojson.feature(topology, topology.objects.countries);
const output = path.resolve(__dirname, '../data/reference/countries_50m.geojson');
fs.mkdirSync(path.dirname(output), { recursive: true });
fs.writeFileSync(output, JSON.stringify({
  type: 'FeatureCollection',
  name: 'world-atlas countries-50m; Natural Earth 50m',
  features: featureCollection.features.map((feature) => ({
    type: 'Feature',
    id: String(feature.id ?? ''),
    properties: { name: feature.properties?.name ?? 'Unknown' },
    geometry: feature.geometry,
  })),
}));
process.stdout.write(`${output}\nfeatures=${featureCollection.features.length}\n`);
