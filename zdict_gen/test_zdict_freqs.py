#!/usr/bin/python3.3

import zdict_freqs as zdf
import os
import tempfile
import unittest

test_data1 = '''Lorem ipsum dolor sit amet, eu habitant ac odio integer ac
odio. Cras dolor tempus ultrices, adipiscing commodi in, morbi sed, nec
vestibulum urna. Vel scelerisque amet. Aliquet ridiculus, nec et diam nostra
commodo. Blandit vitae quam maecenas. Rhoncus morbi mauris, faucibus massa
velit sollicitudin sociosqu, in nec adipiscing. Dolor elit phasellus, suscipit
porttitor euismod nunc, vel in ridiculus sem amet turpis massa. Vestibulum
pulvinar consectetuer tortor lobortis, magna dictum libero egestas enim,
lectus ullamcorper ultricies, ipsum cursus vel tempus, ut a in. Mauris ut arcu
qui vestibulum duis, lacinia ultrices non sed, aut dolor nunc, ridiculus id,
amet litora vel diam. Integer est sodales nec faucibus.'''

test_data2 = '''Lorem ipsum dolor sit amet, eu habitant ac odio integer ac
odio. Cras dolor tempus ultrices, adipiscing commodi in, morbi sed, nec
urna. Vel scelerisque amet. Aliquet ridiculus, nec et diam nostra
commodo. Blandit vitae quam maecenas. Rhoncus morbi mauris, faucibus massa
velit sollici '''

test_fname1 = 'Pamplemousse'
test_fname2 = 'LaCroix'

# generate using http://www.miraclesalad.com/webtools/md5.php
fname_md5 = '259dabbf4c050b3db874eb00d2a5dabb'


class TestUpdatePinZDict(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        zdict_set = zdf.updatePinZDict(test_data1, test_data2)
        self.zdict_set_str = ''.join(zdict_set)

    def test_simple_match(self):
        """
        Common word should be included
        """
        self.assertTrue('amet' in self.zdict_set_str)

    def test_no_non_common_match(self):
        """
        Word not common to both should not be included
        """
        self.assertFalse('litora' in self.zdict_set_str)

    def test_no_self_match(self):
        """
        Word should not be counted as matched if it is only present multiple
        times in its own dataset and not in the other
        """
        self.assertFalse('vestibulum' in self.zdict_set_str)


class TestGetMD5(unittest.TestCase):

    def test_correct_md5(self):
        """
        MD5 hash method should match expected MD5
        """
        self.assertEqual(fname_md5, zdf.getMD5(test_fname1, test_fname2))


class TestRestoreState(unittest.TestCase):

    def test_invalid_file_format(self):
        """
        An input state file with invalid format should throw an Exception
        """
        tf = tempfile.NamedTemporaryFile(mode='w+', delete=False)
        try:
            tf.write('{0}\n{1}\n{2}\nPin'.format(fname_md5, 4, "{ }"))
            tf.flush()
            tf.seek(0)
            self.assertRaises(Exception, zdf.restoreState,
                              test_fname1, test_fname2, tf.name)
        finally:
            tf.close()
            os.remove(tf.name)

    def test_wrong_md5(self):
        """
        An input state file with wrong MD5 hash should throw an Exception
        """
        tf = tempfile.NamedTemporaryFile(mode='w+', delete=False)
        wrong_md5 = '2a4a2782c0782d65f7a9d2ff5fe7a638'  # md5 of 'Wrong hash'
        try:
            tf.write('{0}\n{1}\n{2}'.format(wrong_md5, 4, "{ }"))
            tf.flush()
            tf.seek(0)
            self.assertRaises(Exception, zdf.restoreState,
                              test_fname1, test_fname2, tf.name)
        finally:
            tf.close()
            os.remove(tf.name)

    def test_invalid_line_num(self):
        """
        An input state file with an invalid line number should throw an 
        Exception
        """
        tf = tempfile.NamedTemporaryFile(mode='w+', delete=False)
        try:
            tf.write('{0}\n{1}\n{2}'.format(fname_md5, 'not a num', "{ }"))
            tf.flush()
            tf.seek(0)
            self.assertRaises(Exception, zdf.restoreState,
                              test_fname1, test_fname2, tf.name)
        finally:
            tf.close()
            os.remove(tf.name)

    def test_invalid_json(self):
        """
        An input state file with invalid json should throw an Exception
        """
        tf = tempfile.NamedTemporaryFile(mode='w+', delete=False)
        try:
            tf.write('{0}\n{1}\n{2}'.format(fname_md5, '4', "{ )"))
            tf.flush()
            tf.seek(0)
            self.assertRaises(Exception, zdf.restoreState,
                              test_fname1, test_fname2, tf.name)
        finally:
            tf.close()
            os.remove(tf.name)

if __name__ == '__main__':
    unittest.main()
