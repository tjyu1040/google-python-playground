# -*- coding: utf-8 -*-
from __future__ import division, print_function

import argparse
import glob
import logging
import os
import subprocess
import sys


logger = logging.getLogger(__name__)


def compile_protocol_buffers(src_dir, dest_dir):
    """
    Compile protocol buffers.

    Args:
        src_dir (str): The source directory containing *.proto files to be
            compiled.
        dest_dir (st): The destination directory where the compiled and
            generated classes are stored.
    """
    compile_cmd = ['protoc', '--proto_path', src_dir, '--python_out', dest_dir]
    proto_files = glob.glob(os.path.join(src_dir, '*.proto'))
    if not proto_files:
        logger.error('No *.proto files found in {}'.format(src_dir))
        sys.exit(1)

    compile_cmd.extend(proto_files)
    compile_process = subprocess.Popen(
        compile_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    stdout, stderr = compile_process.communicate()
    if compile_process.returncode != 0:
        logger.error(stderr)
        sys.exit(1)
    else:
        msg = 'Compiled successfully. See {} for generated files.'
        logger.info(msg.format(dest_dir))
        print(msg.format(dest_dir))


def main(argv=None):
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        '-i', '--input-dir', default='./', type=str,
        help='Directory containing .proto files to compile.'
    )
    parser.add_argument(
        '-o', '--output-dir', default='./', type=str,
        help='Directory to write compiled files to.'
    )
    args = parser.parse_args(args=argv)

    input_dir = os.path.abspath(os.path.expanduser(args.input_dir))
    output_dir = os.path.abspath(os.path.expanduser(args.output_dir))
    compile_protocol_buffers(input_dir, output_dir)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    generated_dir = '../_generated'
    main(argv=['-i', generated_dir, '-o', generated_dir])
