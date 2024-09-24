#!/usr/bin/env python
# _*_ coding:utf-8 _*_
# __created by junxi__

# The script is used by python tab Completion script

import readline
import rlcompleter
import os
import atexit

### Indenting
class TabCompleter(rlcompleter.Completer):
    """Completer that supports indenting"""
    def complete(self, text, state):
        if not text:
            return ('    ', None)[state]
        else:
            return rlcompleter.Completer.complete(self, text, state)

readline.set_completer(TabCompleter().complete)

### Add autocompletion
if 'libedit' in readline.__doc__:
    readline.parse_and_bind("bind -e")
    readline.parse_and_bind("bind '\t' rl_complete")
else:
    readline.parse_and_bind("tab: complete")

### Add history
# Use 'HOME' for Unix-based systems and 'USERPROFILE' for Windows
home_dir = os.environ.get("HOME") or os.environ.get("USERPROFILE")

if home_dir:
    histfile = os.path.join(home_dir, ".pyhist")
else:
    # Fallback: If no HOME or USERPROFILE, use current directory
    histfile = ".pyhist"

try:
    readline.read_history_file(histfile)
except IOError:
    pass

# Save history on exit
atexit.register(readline.write_history_file, histfile)
del histfile
