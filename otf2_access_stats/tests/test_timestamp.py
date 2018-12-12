import unittest
import otf2_access_stats.spacecollection as sc


class TestTimeStamp(unittest.TestCase):

    def test_init(self):
        res = 1000000
        t1 = 1000000
        t = sc.TimeStamp(resolution=res, ticks=t1)
        self.assertEqual(t.sec(), 1)
        self.assertEqual(t.usec(), res)


    def test_sub(self):
        res = 1000000
        tstep = 1000000
        t1 = sc.TimeStamp(resolution=res, ticks=tstep * 2)
        t2 = sc.TimeStamp(resolution=res, ticks=tstep * 4)
        dt = t2 - t1
        self.assertEqual(dt.sec(), 2)


    def test_add(self):
        res = 1000000
        tstep = 1000000
        t1 = sc.TimeStamp(resolution=res, ticks=tstep * 2)
        t2 = sc.TimeStamp(resolution=res, ticks=tstep * 4)
        self.assertEqual((t2 + t1).sec(), 6)
        t2 += t1
        self.assertEqual(t2.sec(), 6)



    def test_lt(self):
        res = 1000000
        tstep = 1000000
        t1 = sc.TimeStamp(resolution=res, ticks=tstep * 2)
        t2 = sc.TimeStamp(resolution=res, ticks=tstep * 4)
        self.assertTrue(t1 < t2)


if __name__ == '__main__':
    unittest.main()