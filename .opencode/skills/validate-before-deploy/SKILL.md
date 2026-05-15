# Validation Before Deploy

**Always run validation before committing or deploying any dashboard changes.**

## When to activate
Whenever editing `dashboard/static/index.html` or any Python files.

## Steps

1. **Check JavaScript syntax** — the most common cause of dashboard breakage:
   ```bash
   bash scripts/validate-dashboard.sh
   ```

2. **Check Python syntax** for all modified `.py` files:
   ```bash
   for f in $(git diff --cached --name-only | grep '\.py$'); do
       python3 -c "import ast; ast.parse(open('$f').read())" && echo "✅ $f" || echo "❌ $f"
   done
   ```

3. **Check for duplicate code** in the HTML file — the most frequent bug:
   - Look for repeated function bodies
   - Ensure each `function` definition has exactly one closing `}`
   - Use brace counting: opens `{` should equal closes `}` within each function

4. **Check YAML config**:
   ```bash
   python3 -c "import yaml; yaml.safe_load(open('config/config.yaml'))" && echo "✅ config.yaml"
   ```

5. **Commit hook**: The pre-commit hook at `.git/hooks/pre-commit` runs validation automatically. Install it on each fresh clone:
   ```bash
   cp .opencode/skills/validate-before-deploy/pre-commit .git/hooks/pre-commit
   chmod +x .git/hooks/pre-commit
   ```

## What can break the dashboard
- **Orphaned braces** (extra `}` or missing `}`) — breaks all JavaScript
- **Duplicate function bodies** — causes syntax errors at the duplicate boundary
- **Mismatched try/catch** — try without catch, or extra catch closing
- **`Function constructor errors`** — using browser globals (`location`) in Node tests is fine, but actual JS syntax errors are not

## Verification after deploy
```bash
curl -s http://localhost:8000/ | node -e "
const c = require('fs').readFileSync('/dev/stdin','utf8');
const s = c.indexOf('<script>') + 8, e = c.lastIndexOf('</script>');
new Function('location', c.substring(s, e));
console.log('✅ JavaScript valid');
"
curl -s http://localhost:8000/api/status | python3 -c "import sys,json; d=json.load(sys.stdin); print('✅ Bot online' if d.get('online') else '❌ Offline')"
```
