URL Functions
=============

*[protocol:]//host[:port]][path][?query][#fragment]*

``url_extract_fragment(col)``: Returns the fragment identifier from url.

``url_extract_host(col)``: Returns the host from url.

``url_extract_path(col)``: Returns the path from url.

``url_extract_port(col)``: Returns the port number from url.

``url_extract_protocol(col)``: Returns the protocol from url.

``url_extract_query(col)``: Returns the query string from url.

``url_encode(col)``:

``url_decode(col)``: Unescapes the URL encoded value. This function is
the inverse of url_encode().
