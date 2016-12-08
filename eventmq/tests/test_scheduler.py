# This file is part of eventmq.
#
# eventmq is free software: you can redistribute it and/or modify it under the
# terms of the GNU Lesser General Public License as published by the Free
# Software Foundation, either version 2.1 of the License, or (at your option)
# any later version.
#
# eventmq is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with eventmq.  If not, see <http://www.gnu.org/licenses/>.
import unittest

import mock
import json
import mockredis

from .. import conf, constants, scheduler

ADDR = 'inproc://pour_the_rice_in_the_thing'


class TestCase(unittest.TestCase):
    def test__setup(self):
        sched = scheduler.Scheduler(name='RuckusBringer')
        self.assertEqual(sched.name, 'RuckusBringer')

        self.assertFalse(sched.awaiting_startup_ack)
        self.assertEqual(sched.status, constants.STATUS.ready)

    def test_load_jobs(self):
        sched = scheduler.Scheduler()

        sched.load_jobs()

    @mock.patch('eventmq.scheduler.Scheduler.process_message')
    @mock.patch('eventmq.scheduler.Sender.recv_multipart')
    @mock.patch('eventmq.scheduler.Poller.poll')
    @mock.patch('eventmq.scheduler.Scheduler.maybe_send_heartbeat')
    def test__start_event_loop(self, maybe_send_hb_mock, poll_mock,
                               sender_mock, process_msg_mock):

        sched = scheduler.Scheduler()
        maybe_send_hb_mock.return_value = False
        poll_mock.return_value = {sched.outgoing: scheduler.POLLIN}
        sender_mock.return_value = [1, 2, 3]

        sched._start_event_loop()

    @mock.patch('redis.StrictRedis', mockredis.mock_strict_redis_client)
    def test_on_schedule(self):
        sched = scheduler.Scheduler()
        msg = [
            'default',
            '',
            '3',
            json.dumps(['run', {'path': 'test',
                                'class_args': [0],
                                'callable': 'do_the_thing'}]),
            None
        ]
        sched.on_schedule('1234', msg)

    def test_get_run_couunt_from_headers(self):
        sched = scheduler.Scheduler()
        self.assertEquals(3, sched.get_run_count_from_headers('run_count:3'))
        self.assertEquals(3, sched.get_run_count_from_headers('gaurentee,run_count:3'))
        self.assertEquals(4, sched.get_run_count_from_headers('gaurentee,run_count:4'))

    @mock.patch('redis.StrictRedis', mockredis.mock_strict_redis_client)
    def test_schedule_unschedule_with_redis(self):
        sched = scheduler.Scheduler()
        msg_0 = [
            'default',
            '',
            '3',
            json.dumps(['run', {'path': 'test',
                                'class_args': [0],
                                'callable': 'do_the_thing'}]),
            None
        ]
        sched.on_schedule('1234', msg_0)
        self.assertEquals(1, len(sched.interval_jobs))
        self.assertIsNotNone(sched.redis_server)

        sched.interval_jobs = {}
        sched.load_jobs()
        self.assertEquals(1, len(sched.interval_jobs))

        # Add a duplicate scheduled job
        sched.on_schedule('1234', msg_0)
        self.assertEquals(1, len(sched.interval_jobs))

        msg_1 = [
            'default',
            '',
            '3',
            json.dumps(['run', {'path': 'test',
                                'class_args': [1],
                                'callable': 'do_the_thing'}]),
            None
        ]
        sched.on_schedule('1234', msg_1)

        self.assertEquals(2, len(sched.interval_jobs))

        sched.interval_jobs = {}
        sched.load_jobs()

        self.assertEquals(2, len(sched.interval_jobs))
        sched.on_unschedule('12345', msg_1)
        self.assertEquals(1, len(sched.interval_jobs))
        sched.on_unschedule('12345', msg_0)
        self.assertEquals(0, len(sched.interval_jobs))

    @mock.patch('redis.StrictRedis', mockredis.mock_strict_redis_client)
    def test_schedule_unschedule_cron_with_redis(self):
        sched = scheduler.Scheduler()
        msg_0 = [
            'default',
            '',
            -1,
            json.dumps(['run', {'path': 'test',
                                'class_args': [0],
                                'callable': 'do_the_thing'}]),
            '* * * * *'
        ]
        sched.on_schedule('1234', msg_0)
        self.assertEquals(1, len(sched.interval_jobs))
        self.assertIsNotNone(sched.redis_server)

        sched.interval_jobs = {}
        sched.load_jobs()
        self.assertEquals(1, len(sched.interval_jobs))

        # Add a duplicate scheduled job
        sched.on_schedule('1234', msg_0)
        self.assertEquals(1, len(sched.interval_jobs))

        msg_1 = [
            'default',
            '',
            -1,
            json.dumps(['run', {'path': 'test',
                                'class_args': [1],
                                'callable': 'do_the_thing'}]),
            '* * * * *'
        ]
        sched.on_schedule('1234', msg_1)

        self.assertEquals(2, len(sched.interval_jobs))

        sched.interval_jobs = {}
        sched.load_jobs()

        self.assertEquals(2, len(sched.interval_jobs))
        sched.on_unschedule('12345', msg_1)
        self.assertEquals(1, len(sched.interval_jobs))
        sched.on_unschedule('12345', msg_0)
        self.assertEquals(0, len(sched.interval_jobs))

        sched.on_disconnect()
