#!/bin/bash

# Pre-commit security hook for Claude Code
# This hook runs before any tool use to check for security issues

echo "= Running security checks..."

# Check for hardcoded secrets
if grep -rE "(api_key|password|secret|token)\s*=\s*['\"][^'\"]+['\"]" --include="*.py" . 2>/dev/null; then
    echo "�  Warning: Potential hardcoded secrets detected!"
    echo "Please use environment variables or secure credential storage."
fi

# Check for sensitive file operations
if [[ "$1" == *"Write"* ]] || [[ "$1" == *"Edit"* ]]; then
    if [[ "$2" == *".env"* ]] || [[ "$2" == *"credentials"* ]] || [[ "$2" == *"secrets"* ]]; then
        echo "�  Warning: Attempting to modify sensitive files!"
        echo "Please ensure no secrets are being exposed."
    fi
fi

# Log the operation
mkdir -p .claude/logs
echo "[$(date)] Tool: $1, Target: $2" >> .claude/logs/security.log

echo " Security checks completed"