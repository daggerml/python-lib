from daggerml._internal.codec import CodecContext, register_codec


def test_internal_codec_exports_available():
    assert callable(register_codec)
    assert CodecContext is not None
