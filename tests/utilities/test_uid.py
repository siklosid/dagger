from unittest import TestCase
from unittest.mock import Mock, PropertyMock
from dagger.utilities import uid


class TestUid(TestCase):
    def test_uid(self):
        object_id = "object_id"
        other_object_id = "other_object_id"

        expected_uid = 'af31437ce61345f416579830a98c91e5'
        self.assertEqual(uid.get_uid(object_id), expected_uid)

        # Check if the same string generates the same hash
        self.assertEqual(uid.get_uid(object_id), expected_uid)

        # Check if different string generates different hash
        self.assertNotEqual(uid.get_uid(other_object_id), expected_uid)

    def test_get_pipeline_uid(self):
        pipeline_mock = Mock()
        type(pipeline_mock).name = PropertyMock(return_value="pipeline_name")

        self.assertEqual(uid.get_pipeline_uid(pipeline_mock), uid.get_uid("pipeline_name"))

    def test_get_task_uid(self):
        task_mock = Mock()
        type(task_mock).name = PropertyMock(return_value="task_name")
        type(task_mock).pipeline_name = PropertyMock(return_value="pipeline_name")

        self.assertEqual(uid.get_task_uid(task_mock), uid.get_uid("pipeline_nametask_name"))

    def test_get_dataset_uid(self):
        attrs = {'alias.return_value': 'dataset_alias', 'other.side_effect': KeyError}
        dataset_mock = Mock(**attrs)

        self.assertEqual(uid.get_dataset_uid(dataset_mock), uid.get_uid("dataset_alias"))
