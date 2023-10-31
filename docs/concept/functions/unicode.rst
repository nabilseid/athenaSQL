Unicode Functions
=================

``normalize(col)``: Transforms string with NFC normalization form.

``to_utf8(col)``: Encodes string into a UTF-8 varbinary representation.

``from_utf8(col)``: Decodes a UTF-8 encoded string from binary. Invalid
UTF-8 sequences are replaced with the Unicode replacement character
U+FFFD.