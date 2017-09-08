from unittest.mock import MagicMock
import unittest
from pqueue import Queue

import server


class TestServerFailover(unittest.TestCase):
    """
    TODO(geoff):

    1. It looks like the test isn't closing the file resources, and it is complaining about this.
       This isn't an issue for production, but is for the tests.
    2. these shouldn't use the production file directories. They should use a test based one.

    """

    fake_requests = [
        "Not a real request",
        "Also not a real request"
    ]

    def queue_persistence(self, queue, tdir, qdir):

        self.assertEqual(queue.qsize(), 0)
        for request in self.fake_requests:
            queue.put(request)
        self.assertEqual(queue.qsize(), len(self.fake_requests))

        del queue
        queue = Queue(qdir, tempdir=tdir)

        self.assertEqual(queue.qsize(), len(self.fake_requests))
        for _ in range(len(self.fake_requests)):
            queue.get()
        self.assertEqual(queue.qsize(), 0)

    def test_queues(self):

        queues_and_dirs = {
            server.originality_queue : [server.TEMP_DIR, server.OQ_DIR],
            server.concordance_queue : [server.TEMP_DIR, server.CQ_DIR],
        }

        for queue, dirs in queues_and_dirs.items():
            self.queue_persistence(queue, dirs[0], dirs[1])






if __name__ == '__main__':
    unittest.main()
