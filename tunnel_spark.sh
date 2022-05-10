#!/usr/bin/bash
ssh -fN -L 7077:localhost:7077 -L 8080:localhost:8080 -L 8081:localhost:8081 -L 4040:localhost:4040 dif06.idi.ntnu.no
