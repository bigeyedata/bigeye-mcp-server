#!/bin/bash
exec docker run --rm -i -v ${HOME}/.bigeye-mcp:/home/mcp/.bigeye-mcp bigeye-mcp-server:latest python server.py