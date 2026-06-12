from daggerml import Uri
from daggerml.contrib.testing import MockNode


def test_contrib_testing_001__mocknode_from_value_preserves_node_like_inputs():
    node = MockNode(7)
    assert MockNode.from_value(node) is node
    assert MockNode.from_value(3).value() == 3
    assert MockNode(Uri("s3://bucket/path")).value().uri == "s3://bucket/path"
