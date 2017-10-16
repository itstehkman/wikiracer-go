# wikiracer-go
wiki + racer + go. go figure...

Ever heard of wiki racing? No? Cool, check this out: https://en.wikipedia.org/wiki/Wikipedia:Wikirace<br>

Some cool things I had to do was client side rate limiting so I didn't bog the f*** out of wikipedia's API. So <br>
This also brought up the idea of a client side request queue, which also handles callbacks for when the request is finished.<br>
Kind of an interesting pattern. I think I could have done it bit better, I often find that the rate limit is not really hit at all, <br>
but I'll still get 429 response codes back. Will take a look at this later.
