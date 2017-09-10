#!/usr/bin/env python
from fdfgen import forge_fdf

fields = [('model','some_model')]
fdf = forge_fdf("",fields,[],[],[])
fdf_file = open("data.fdf","wb")
fdf_file.write(fdf)
fdf_file.close()
