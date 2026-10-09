"""Fingerprint reviewable source/config/docs without secrets or generated data."""
import hashlib
import json
from pathlib import Path
import subprocess

root = Path('/Users/brybry.o_o/perennial/PerennialBackend')
paths = subprocess.check_output(['git', 'ls-files', '-z', '--cached', '--others', '--exclude-standard'], cwd=root).decode().split('\0')

def included(name):
    path = Path(name)
    if name in {'README.md', 'requirements.txt', 'pytest.ini', 'development/backend/pytest.ini', 'development/backend/.env.example', 'development/database/.env.example'}:
        return True
    if name.startswith('development/backend/src/'):
        return path.suffix in {'.py', '.md'}
    if name.startswith('development/backend/tests/'):
        return path.suffix in {'.py', '.json', '.ini'}
    if name.startswith('development/database/') and '/local_data/' not in name:
        return path.suffix in {'.py', '.ini', '.md', '.yml', '.yaml', '.sql', '.mako'}
    if name.startswith('docs/technical-specs/'):
        return path.suffix == '.md'
    if name.startswith('.github/workflows/'):
        return path.suffix in {'.yml', '.yaml'}
    return False

files = {name: hashlib.sha256((root/name).read_bytes()).hexdigest()
         for name in sorted(set(paths)) if name and included(name) and (root/name).is_file()}
encoded = ''.join(f'{digest}  {name}\n' for name, digest in files.items()).encode()
result = {'head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root).decode().strip(),
          'sha256': hashlib.sha256(encoded).hexdigest(), 'file_count': len(files), 'files': files}
print(json.dumps(result, indent=2))
