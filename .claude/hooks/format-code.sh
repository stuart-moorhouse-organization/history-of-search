#!/bin/bash

# Format code hook for Claude Code
# This hook runs after file writes to ensure code formatting

echo "🎨 Running code formatter..."

# Detect file type and format accordingly
FILE_PATH="$1"
FILE_EXT="${FILE_PATH##*.}"

case "$FILE_EXT" in
    py)
        # Format Python files
        if command -v black &> /dev/null; then
            black "$FILE_PATH" 2>/dev/null
            echo "✅ Formatted with Black"
        fi
        
        # Sort imports
        if command -v isort &> /dev/null; then
            isort "$FILE_PATH" 2>/dev/null
            echo "✅ Sorted imports with isort"
        fi
        ;;
    toml|yaml|yml|json)
        # Format configuration files if prettier is available
        if command -v prettier &> /dev/null; then
            prettier --write "$FILE_PATH" 2>/dev/null
            echo "✅ Formatted with Prettier"
        fi
        ;;
    md)
        # Format markdown files if prettier is available
        if command -v prettier &> /dev/null; then
            prettier --write "$FILE_PATH" 2>/dev/null
            echo "✅ Formatted with Prettier"
        fi
        ;;
    *)
        echo "ℹ️  No formatter configured for .$FILE_EXT files"
        ;;
esac

# Log the formatting action
mkdir -p .claude/logs
echo "[$(date)] Formatted: $FILE_PATH" >> .claude/logs/formatting.log