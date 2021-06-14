# nuxeo-sandbox (LTS 2019)
nuxeo mle sandbox built with https://doc.nuxeo.com/nxdoc/nuxeo-cli/#multiple-modules-empty-nuxeo-project :

```bash
# > parent + core :
nuxeo bootstrap multi-module
#id : com.mlefree.nuxeo.sandbox
#nuxeo version : LTS 2019

# > package :
nuxeo bootstrap package

# > link nuxeo studio mleprevost-SANDBOX :
nuxeo studio
```

## Patterns

### 1. Audit Storage

This module enables to specify one or several storages for the Nuxeo Audit logs.

For instance, the Audit logs can be stored in both Elastic Search and a SQL storage.

#### Configuration

You must define in your `nuxeo.conf`:

    nuxeo.stream.audit.enabled = true

## Contact

@mlefree @nuxeo

[Licence](./LICENSE)

