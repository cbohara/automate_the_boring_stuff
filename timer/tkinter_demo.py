#!/usr/local/bin/python3
import sys
import tkinter as tk


class Application(tk.Frame):
    def __init__(self, master=None):
        tk.Frame.__init__(self, master)
        self.grid()
        self.createWidgets()

    def createWidgets(self):
        self.quitButton = tk.Button(self, text="Quit", command=self.quit)
        self.quitButton.grid()


def main():
    """Build demo application using tkinter."""
    app = Application()
    app.master.title("Sample application")
    app.mainloop()


if __name__ == "__main__":
    sys.exit(main())
