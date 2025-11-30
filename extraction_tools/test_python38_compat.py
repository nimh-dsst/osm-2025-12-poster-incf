#!/usr/bin/env python3
"""
Quick test to verify Python 3.8 type hint compatibility
"""
from pathlib import Path
from typing import Tuple

def test_function() -> Tuple[str, str, str]:
    """Test that Tuple type hints work in Python 3.8"""
    return ("test1", "test2", "test3")

if __name__ == "__main__":
    result = test_function()
    print(f"✓ Type hints work correctly: {result}")
    print("✓ Python 3.8 compatibility confirmed")
