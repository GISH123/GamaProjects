# Pure Python 3.x demo, 256 colors
# Works with bash under Linux and MacOS
'''
# 使用方式
from package.common.common.colorsCtrl import ColorCtrl
colorCtrl = ColorCtrl()
# 頻色列表
print(colorCtrl.show())
# 文字頻色
gameName = "lineageglobal"
print(f"test color string {ColorCtrl.fg(gameName, 160)}")
# 文字背景頻色
print(f"test color string {ColorCtrl.bg('gameName', 160)}")
'''


class ColorCtrl:

    def __init__(self):
        self.__fg = lambda text, color: "\33[38;5;" + str(color) + "m" + text + "\33[0m"
        self.__bg = lambda text, color: "\33[48;5;" + str(color) + "m" + text + "\33[0m"
        self.__end = "\n"

    def show(self):
        for row in range(0, 43):
            for fromat in [self.__fg, self.__bg]:
                for col in range(6):
                    color = row * 6 + col - 2
                    if color >= 0:
                        text = "{:3d}".format(color)
                        print(fromat(text, color), end=" ")
                    else:
                        print(end="    ")  # four spaces
            print(end=self.__end)

    @classmethod
    def fg(self, text, color):
        return "\33[38;5;" + str(color) + "m" + text + "\33[0m"

    @classmethod
    def bg(self, text, color):
        return "\33[48;5;" + str(color) + "m" + text + "\33[0m"
