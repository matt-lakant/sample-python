import os
import http.server
import socketserver

from http import HTTPStatus

import subprocess


class Handler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        # Call the other script and capture its output
        result = subprocess.run(['python3', 'Read_File_From_DOSpace.py'], capture_output=True, text=True)
        # result = subprocess.run(['python3', 'Read_File_From_Local.py'], capture_output=True, text=True)

        # Prepare the response message
        if result.returncode != 0:
            msg = f"Error running other script: {result.stderr}"
        else:
            msg = f"Output from other script: {result.stdout.strip()}"

        # Send HTTP response
        self.send_response(HTTPStatus.OK)
        self.end_headers()
        self.wfile.write(msg.encode())

port = int(os.getenv('PORT', 80))
print('Listening on port %s' % (port))
httpd = socketserver.TCPServer(('', port), Handler)
httpd.serve_forever()
