
import sys
from typing import Dict, List, Optional

class JobArgsSetup:
    """
    Handles argument parsing for AWS Glue jobs.
    Uses awsglue.utils.getResolvedOptions if available, otherwise falls back to argparse or defaults.
    """
    def __init__(self, required_args: List[str] = None, force_local: bool = False, local_args: Dict[str, str] = None):
        """
        :param required_args: List of required argument names (without -- prefix)
        :param force_local: If True, skip AWS Glue detection and use local_args
        :param local_args: Dictionary of hardcoded arguments for local development
        """
        self.args: Dict[str, str] = {}
        self.required_args = required_args or []
        self.force_local = force_local
        self.local_args = local_args or {}
        self._parse_args()

    def _parse_args(self):
        # Force local mode with hardcoded args
        if self.force_local:
            print("Running in FORCED local mode with hardcoded arguments.")
            self.args = self.local_args
            return

        try:
            from awsglue.utils import getResolvedOptions
            if not self.required_args:
                 self.args = {
                     "env": "local"
                 }
                 return

            self.args = getResolvedOptions(sys.argv, self.required_args)
        except ImportError:
            # Fallback for local testing
            print("Running in local mode (AWS Glue not detected). Using mock/system args.")
            self._parse_local_args()
        except Exception as e:
            print(f"Error parsing arguments: {e}")
            raise e

    def _parse_local_args(self):
        """
        Simple local parser or mock. Strips '--' prefix from argument names.
        """
        import argparse
        parser = argparse.ArgumentParser()
        for arg in self.required_args:
            parser.add_argument(f"--{arg}", default=None)
        
        parsed, _ = parser.parse_known_args()
        raw_args = vars(parsed)
        
        # Strip '--' prefix and clean up argument names
        self.args = {key.lstrip('-'): value for key, value in raw_args.items()}

    def get(self, key: str, default: Optional[str] = None) -> str:
        return self.args.get(key, default)
