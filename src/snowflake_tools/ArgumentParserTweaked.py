import argparse
from argparse import HelpFormatter
from gettext import gettext as _
import sys as _sys


# This is a tweak related to the argparse "exit on error" issue described here:
# https://stackoverflow.com/questions/67890157/python-3-9-1-argparse-exit-on-error-not-working-in-certain-situations


class ArgumentParserTweaked(argparse.ArgumentParser):
    def __init__(
        self,
        prog=None,
        usage=None,
        description=None,
        epilog=None,
        parents=[],
        formatter_class=HelpFormatter,
        prefix_chars="-",
        fromfile_prefix_chars=None,
        argument_default=None,
        conflict_handler="error",
        add_help=True,
        allow_abbrev=True,
        exit_on_error=True,
    ):

        super().__init__(
            prog,
            usage,
            description,
            epilog,
            parents,
            formatter_class,
            prefix_chars,
            fromfile_prefix_chars,
            argument_default,
            conflict_handler,
            add_help,
            allow_abbrev,
            exit_on_error,
        )

    def error(self, message):
        """error(message: string)

        Prints a usage message incorporating the message to stderr and
        exits.

        If you override this in a subclass, it should not return -- it
        should either exit or raise an exception.
        """

        if self.exit_on_error is True:
            self.print_usage(_sys.stderr)
            args = {"prog": self.prog, "message": message}
            self.exit(2, _("%(prog)s: error: %(message)s\n") % args)
        else:
            raise Exception(message)
