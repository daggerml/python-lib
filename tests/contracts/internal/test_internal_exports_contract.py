from daggerml._internal import CodecContext, apply_codec, register_codec


def test_internal_codec_exports_available():
    assert callable(apply_codec)
    assert callable(register_codec)
    assert CodecContext is not None
