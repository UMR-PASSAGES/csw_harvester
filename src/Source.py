# -*- coding: utf-8 -*-


class Source:
    """

    This module allows manipulate each geocatalog and his attributes as un object.

    """

    def __init__(self, num_idg=None, name_idg=None, begin_record=None, end_record=None, step=None, url_idg=None, url_csw=None):
        """

        :param num_idg: the number of IDG ( must be uniq)
        :param name_idg: the name of IDG
        :param begin_record: the first record to get
        :param end_record: the last record to get
        :param step: the data range
        :param url_idg: The URL where we find more information about IDG
        :param url_csw: The url to get geographic data.
        
        Test:
        >>> source = Source(1, "test", 0, 3, 1, "test.com", "test_csw.com")
        >>> source.num_idg == 1
        True
        >>> source.name_idg == "test"
        True
        >>> source.begin_record == 0
        True
        >>> source.end_record == 3
        True
        >>> source.step == 1
        True
        >>> source.url_idg == "test.com"
        True
        >>> source.url_csw == "test_csw.com"
        True
        """
        self.num_idg = num_idg
        self.name_idg = name_idg
        self.begin_record = begin_record
        self.end_record = end_record
        self.step = step
        self.url_idg = url_idg
        self.url_csw = url_csw

if __name__ == "__main__":
    import doctest
    doctest.testmod()
