"""
create_admin.py — One-time script to create the first admin account.

Usage:
    python3 create_admin.py
    python3 create_admin.py --email admin@bnr.rw --name "Admin User"

Only @bnr.rw addresses are accepted.
"""
import argparse
import getpass
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import dq_auth


def main() -> None:
    parser = argparse.ArgumentParser(description="Create a BNR DQ Dashboard admin account")
    parser.add_argument("--email", default=None, help="BNR email address (must be @bnr.rw)")
    parser.add_argument("--name",  default=None, help="Full name")
    parser.add_argument("--role",  default="admin", choices=["admin", "viewer"])
    args = parser.parse_args()

    print("BNR Data Quality Dashboard — Create User")
    print("─" * 42)

    email = args.email or input("Email (@bnr.rw): ").strip()
    if not dq_auth.is_valid_bnr_email(email):
        print(f"Error: email must be @bnr.rw — got {email!r}")
        sys.exit(1)

    name = args.name or input("Full name: ").strip()
    if not name:
        print("Error: name cannot be empty")
        sys.exit(1)

    password = getpass.getpass("Password (min 8 chars): ")
    confirm  = getpass.getpass("Confirm password: ")
    if password != confirm:
        print("Error: passwords do not match")
        sys.exit(1)

    try:
        dq_auth.create_user(email, name, password, role=args.role)
        print(f"\n✓ User created: {email} ({args.role})")
        print("You can now log in to the BNR DQ Dashboard.")
    except ValueError as exc:
        print(f"Error: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
