from app.translators.pfb_to_rawls import PFBToRawls

# file-like to ([Entity])
def test_translate_pfb_file_with_array_to_entities(fake_import, fake_pfb_with_array):

    translator = PFBToRawls()

    entities = translator.translate(fake_import, fake_pfb_with_array)
    assert(list(entities) == "hello")
