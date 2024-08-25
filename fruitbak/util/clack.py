#! /usr/bin/python3

import argparse


class clack:
    _parser = None
    _subparsers = None

    def __init__(self, func, parser=None):
        self._func = func
        self._parser = parser
        self._stack = []

    def command(self, *args, name=None, **kwargs):
        def command(func):
            subparsers = self._subparsers
            if subparsers is None:
                subparsers = self._parser.add_subparsers(description='')
                self._subparsers = subparsers
            stack = self._stack
            self._stack = []
            command_name = func.__name__ if name is None else name
            parser = subparsers.add_parser(command_name, *args, **kwargs)
            c = clack(func, parser)
            for s in reversed(stack):
                s(c)
            parser.set_defaults(_func=func)
            return c

        return command

    def argument(self, *args, **kwargs):
        def argument(func):
            def add_argument(self):
                self._parser.add_argument(*args, **kwargs)

            self._stack.append(add_argument)
            return func

        return argument

    def subparsers(self, *args, **kwargs):
        def subparsers(func):
            def add_subparsers(self):
                self._subparsers = self._parser.add_subparsers(*args, **kwargs)

            self._stack.append(add_subparsers)
            return func

        return subparsers

    def defaults(self, *args, **kwargs):
        def defaults(func):
            def set_defaults(self):
                self._parser.set_defaults(*args, **kwargs)

            self._stack.append(set_defaults)
            return func

        return defaults

    def __call__(self, *args, **kwargs):
        ns = self._parser.parse_args(*args, **kwargs)
        nsd = vars(ns).copy()
        func = nsd.pop('_func')
        return func(**nsd)


def command(*args, **kwargs):
    def command(func_or_clack):
        parser = argparse.ArgumentParser(*args, **kwargs)
        if isinstance(func_or_clack, clack):
            c = func_or_clack
            c._parser = parser
            for s in reversed(c._stack):
                s(c)
            c._stack = []
        else:
            c = clack(func_or_clack, parser)
        parser.set_defaults(_func=c._func)
        return c

    return command


def argument(*args, **kwargs):
    def argument(func_or_clack):
        if isinstance(func_or_clack, clack):
            c = func_or_clack
        else:
            c = clack(func_or_clack)

        def add_argument(self):
            self._parser.add_argument(*args, **kwargs)

        c._stack.append(add_argument)
        return c

    return argument


def subparsers(*args, **kwargs):
    def subparsers(func_or_clack):
        if isinstance(func_or_clack, clack):
            c = func_or_clack
        else:
            c = clack(func_or_clack)

        def add_subparsers(self):
            self._subparsers = self._parser.add_subparsers(*args, **kwargs)

        c._stack.append(add_subparsers)
        return c

    return subparsers


def defaults(*args, **kwargs):
    def defaults(func_or_clack):
        if isinstance(func_or_clack, clack):
            c = func_or_clack
        else:
            c = clack(func_or_clack)

        def set_defaults(self):
            self._parser.set_defaults(*args, **kwargs)

        c._stack.append(set_defaults)
        return c

    return defaults
