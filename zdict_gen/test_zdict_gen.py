#!/usr/bin/python3.3

import unittest
import zdict_gen

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


class TestGenDictFromFreq(unittest.TestCase):

    def test_is_corrrect_order(self):
        """
        Should correctly score and generate dictionary
        """
        test_counts = {'corgi':  10,  # score 50
                       'cat':     5,  # score 15
                       'lizard':  6,  # score 36
                       'gecko':   7,  # score 35
                       'hamster': 4,  # score 28
                       'parrot':  3}  # score 18
        full_dict = zdict_gen.genDictFromFreq(test_counts, -1)
        expected_dict = 'catparrothamstergeckolizardcorgi'
        self.assertEqual(full_dict, expected_dict)

    def test_equal_to_size_b(self):
        """
        Output of dictionary should be equal to size_b if size_b is valid and
        the unlimited dictionary would have been larger than size_b
        """
        test_counts = {'corgi':  10,  # score 50
                       'cat':     5,  # score 15
                       'lizard':  6,  # score 36
                       'gecko':   7,  # score 35
                       'hamster': 4,  # score 28
                       'parrot':  3}  # score 18

        expected_dict = 'geckolizardcorgi'
        expected_size = len(expected_dict)
        small_dict = zdict_gen.genDictFromFreq(test_counts, expected_size)
        self.assertEqual(small_dict, expected_dict)

    def test_remove_substrings(self):
        """
        Doesn't include substring words in the output dictionary
        """
        test_counts = {'corgi':     10,  # score 50
                       'corgicat':   5}  # score 40

        expected_dict = 'corgicat'
        d = zdict_gen.genDictFromFreq(test_counts, -1)
        self.assertEqual(d, expected_dict)

if __name__ == '__main__':
    unittest.main()
