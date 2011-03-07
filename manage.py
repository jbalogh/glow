#!/usr/bin/env python
import site
import sys

# Add vendor so we can import 3rd-party libs.
site.addsitedir('vendor')

import argparse

import log_settings
import glow


def shell():
    try:
        import IPython
        IPython.Shell.IPShell(argv=[], user_ns={'g': glow}).mainloop()
    except ImportError:
        import code
        code.interact()


COMMANDS = {
    'shell': shell,
    'glow': glow.main,
    'cleanup': glow.cleanup,
}


parser = argparse.ArgumentParser()
parser.add_argument('command', choices=sorted(COMMANDS),
                    help='what should I do?')


if __name__ == '__main__':
    args = parser.parse_args(sys.argv[1:2])
    try:
        COMMANDS[args.command]()
    except KeyboardInterrupt:
        raise
        pass  # Die quietly.
