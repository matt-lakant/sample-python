import os
import http.server
import socketserver

from http import HTTPStatus


class Handler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        self.send_response(HTTPStatus.OK)
        self.end_headers()
        msg = 'Hello! Matthieu you are editing the server.py file %s' % (self.path)
        self.wfile.write(msg.encode())


import subprocess

def call_other_script():
    # Call the other Python script and capture the output
    result = subprocess.run(['python3', 'Load_FTSE_Files.py'], capture_output=True, text=True)
    
    # Check for errors
    if result.returncode != 0:
        print("Error:", result.stderr)
    else:
        print("Output from other script:", result.stdout)

if __name__ == "__main__":
    # Call the other script when the server starts
    call_other_script()

port = int(os.getenv('PORT', 80))
print('Listening on port %s' % (port))
httpd = socketserver.TCPServer(('', port), Handler)
httpd.serve_forever()
