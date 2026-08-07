"""Minimal Luma crawler contract example.

The target website is available as argv[1] and LUMA_TARGET_WEBSITE. Real crawlers
can use urllib or their own installed dependencies, then emit the same JSON shape.
"""

import json
import os
import sys


website = sys.argv[1] if len(sys.argv) > 1 else os.environ["LUMA_TARGET_WEBSITE"]
results = [
    {
        "title": f"Example result from {website}",
        "downloadUrl": "magnet:?xt=urn:btih:0123456789ABCDEF0123456789ABCDEF01234567&dn=LumaExample",
        "trackers": ["udp://tracker.opentrackr.org:1337/announce"],
    }
]

result_path = os.environ.get("LUMA_RESULT_PATH")
if result_path:
    with open(result_path, "w", encoding="utf-8") as output:
        json.dump({"results": results}, output, ensure_ascii=False)
else:
    print(json.dumps({"results": results}, ensure_ascii=False))
