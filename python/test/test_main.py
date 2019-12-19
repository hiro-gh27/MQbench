from unittest import TestCase

from main.main import *


class Test(TestCase):
    def test_check(self):
        logging.basicConfig(level=logging.DEBUG)
        create_if_absent()
        # self.fail()

    def test(self):
        go_src_path = "../../"
        os.chdir(go_src_path)
        test_dir = "20191208_173418508750"
        read_result_then_decide(test_dir)
