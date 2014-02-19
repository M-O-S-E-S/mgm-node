from distutils.core import setup
import py2exe

setup(
    service = ["node"],
    description = "MGM Host Node Service",
    data_files = [("", ["mgm.cfg"])],
    cmdline_style='pywin32',
)
