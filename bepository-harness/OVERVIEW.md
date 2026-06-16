# bepository-harness

Test library that spawns throwaway Go Syncthing instances ("masters") and drives
them over their REST API. Used by interop tests to validate wire-level
compatibility against the real implementation, not mocks.

## Key API

- `Harness::start()` — spawn a Syncthing process (binary from `$SYNCTHING_BIN`,
  else `PATH`) with a temporary config dir and wait for its REST API.
- `share(path)` / `share_named(path, folder_id)` — share a folder.
- `add_peer(device_id, addr)` — connect it to another instance or to bepository.
- Dropping a `Harness` kills the child process and deletes its temp dir.

## Key decisions

- Everything is throwaway: temp dirs, random ports, fresh certs per instance.
- The justfile downloads a Syncthing binary for e2e runs if none is available
  (`USE_SYSTEM_SYNCTHING=0` forces the download).
