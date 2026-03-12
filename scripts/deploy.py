#!/usr/bin/env python3
"""
Lakehouse Federation Demo - Interactive Deploy Script

Usage:
  python scripts/deploy.py              # Interactive deploy
  python scripts/deploy.py --redeploy   # Non-interactive redeploy
  python scripts/deploy.py --destroy    # Destroy all resources
"""

import sys

from deployer import Deployer

if __name__ == "__main__":
    deployer = Deployer()
    if "--destroy" in sys.argv:
        deployer.destroy()
    elif "--redeploy" in sys.argv:
        deployer.redeploy()
    else:
        deployer.deploy()
