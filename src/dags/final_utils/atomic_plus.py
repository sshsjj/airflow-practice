from atomicwrites import atomic_write as _backend_writer, AtomicWriter
import tempfile
from contextlib import contextmanager
import io
import sys
import os
import pathlib


# You probably need to inspect and override some internals of the package
class SuffixWriter(AtomicWriter):

    def get_fileobject(self, dir=None, **kwargs):
    #def get_fileobject(self, suffix = "", prefix = tempfile.template, dir = None, ** kwargs):
        # My research and testing below - as there is no requirement for the suffix

        # Option1: Get the file_extension - will treat 'python.tar.gz's suffix as .gz
        # filename, file_extension = os.path.splitext(self._path)
        # suffix = file_extension

        # Option2: Get the extension through pathlib package - treat 'python.tar.gz''s suffix as .tar.gz
        # Option2 is SELECTED
        suffix = ''.join(pathlib.Path(self._path).suffixes)

        if dir is None:
            dir = os.path.normpath(os.path.dirname(self._path))
        descriptor, name = tempfile.mkstemp(suffix=suffix, prefix=tempfile.template, dir=dir)

        os.close(descriptor)
        kwargs['mode'] = self._mode
        kwargs['file'] = name

        return io.open(**kwargs)
        #return AtomicWriter.get_fileobject(self, suffix=suffix, dir=dir, **kwargs)


@contextmanager
def atomic_write(file, mode='w', as_file=True, new_default='asdf', **kwargs):
    if os.path.isfile(file):
        print("Testing File Exists !!!!!!!!!!!!!!!!!!")
        raise FileExistsError

    with _backend_writer(str(file), writer_cls=SuffixWriter, **kwargs) as f:
        # Handling the as_file logic

        # First check if the file exists or not,
        # if yes, raise FileExistsError/ if not, get the filename and path
        #
        if os.path.isfile(file):
            print("Testing File Exists !!!!!!!!!!!!!!!!!!")
            raise FileExistsError

        if as_file:
            yield f
        else:
            yield f.name


#---------------------------------------------------------------
class BinarySuffixWriter(AtomicWriter):

    def __init__(self, path, mode='wb', overwrite=False, **open_kwargs):

        AtomicWriter.__init__(self, path, mode='wb', overwrite=False, **open_kwargs)
        self._mode = 'wb'

    def get_fileobject(self, dir=None, **kwargs):
        # def get_fileobject(self, suffix = "", prefix = tempfile.template, dir = None, ** kwargs):
        # My research and testing below - as there is no requirement for the suffix

        # Option1: Get the file_extension - will treat 'python.tar.gz's suffix as .gz
        # filename, file_extension = os.path.splitext(self._path)
        # suffix = file_extension

        # Option2: Get the extension through pathlib package - treat 'python.tar.gz''s suffix as .tar.gz
        # Option2 is SELECTED
        suffix = ''.join(pathlib.Path(self._path).suffixes)

        if dir is None:
            dir = os.path.normpath(os.path.dirname(self._path))
        descriptor, name = tempfile.mkstemp(suffix=suffix, prefix=tempfile.template, dir=dir)

        os.close(descriptor)
        kwargs['mode'] = 'wb'
        kwargs['file'] = name

        return io.open(**kwargs)
        # return AtomicWriter.get_fileobject(self, suffix=suffix, dir=dir, **kwargs)



@contextmanager
def atomic_write_b(file, mode='wb', as_file=True, as_content=False, new_default='asdf', **kwargs):
    if os.path.isfile(file):
        print("Testing File Exists !!!!!!!!!!!!!!!!!!")
        raise FileExistsError

    with _backend_writer(str(file), writer_cls=BinarySuffixWriter, **kwargs) as f:
        # Handling the as_file logic

        # First check if the file exists or not,
        # if yes, raise FileExistsError/ if not, get the filename and path
        #
        if os.path.isfile(file):
            print("Testing File Exists !!!!!!!!!!!!!!!!!!")
            raise FileExistsError

        if as_file:
            yield f
        else:
            yield f.name