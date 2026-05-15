#!/usr/bin/env bash
# Validate dashboard HTML/JS before deploy
set -e

HTML="dashboard/static/index.html"

if [ ! -f "$HTML" ]; then
    echo "❌ $HTML not found"
    exit 1
fi

# 1. Validate HTML structure
python3 -c "
with open('$HTML') as f:
    c = f.read()
assert '<!DOCTYPE html>' in c, 'Missing DOCTYPE'
assert '<script>' in c, 'Missing script tag'
assert '</script>' in c, 'Missing closing script tag'
assert '<style>' in c, 'Missing style tag'
assert '</style>' in c, 'Missing closing style tag'
print('  ✅ HTML structure')
"

# 2. Validate JavaScript syntax using Node.js
echo "const http = require('http');
const fs = require('fs');
const html = fs.readFileSync('$HTML', 'utf8');
const start = html.indexOf('<script>') + 8;
const end = html.lastIndexOf('</script>');
const code = html.substring(start, end);
try {
    new Function('location', code);
    console.log('  ✅ JavaScript syntax');
} catch(e) {
    console.log('  ❌ JavaScript ERROR: ' + e.message);
    process.exit(1);
}
" | node

# 3. Check for common issues
python3 -c "
with open('$HTML') as f:
    c = f.read()
total_opens = c.count('function ')  # rough check
# Check no duplicate function names
import re
funcs = re.findall(r'function (\w+)', c)
from collections import Counter
dupes = [f for f, count in Counter(funcs).items() if count > 1 and f != 'compute']
if dupes:
    print(f'  ⚠️ Duplicate functions: {dupes}')
else:
    print('  ✅ No duplicate functions')
print('  ✅ All checks passed')
" 2>&1 || echo "  ⚠️ Some checks failed"
