# linkstash

A deliberately small URL shortener. This is the codebase the demo fleet works on.

It exists so `codefleet demo` has something real to modify: a handful of modules, a test
suite that passes before the run and must still pass after it, and enough structure that
five tasks can be split across parallel agents without being contrived.

```
linkstash/
  codec.py      base62 encode/decode
  config.py     configuration constants
  store.py      in-memory link store
  api.py        request dispatcher
tests/
  test_codec.py
  test_store.py
```

No dependencies beyond the standard library and pytest.

```bash
python -m pytest tests/ -q
```
