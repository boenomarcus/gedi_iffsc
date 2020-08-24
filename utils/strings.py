

def colors(txt, color=None, background=None, font=0):
    
    clr_prefix = "\033[" + str(font)
    if color is not None:
        clr_prefix += ";3" + str(color)
    if background is not None:
        clr_prefix += ";4" + str(background)
    clr_prefix += "m"
    
    return clr_prefix + str(txt) + "\033[m"


def yes_no_input(txt):
    while True:
        a = input(txt)
        if a in "YyNn":
            return a
        print(colors("[ERROR] Confirm or deny [y/n]!", 1))


def greeting_gediFinder():
    print("\n" + "-="*30 + "\n" + "-"*60)
    print(f"{'Welcome to GEDI Finder!':^60}")
    print("-"*60 + "\n" + "-="*30 + "\n")


def greeting_gedi2mongo():
    print("\n" + "-="*30 + "\n" + "-"*60)
    print(f"{'Welcome to GEDI 2 MongoDB!':^60}")
    print("-"*60 + "\n" + "-="*30 + "\n")


def greeting_lidar_mongo():
    print("\n" + "-="*30 + "\n" + "-"*60)
    print(f"{'Welcome to Orbital Lidar to MongoDB!':^60}")
    print("-"*60 + "\n" + "-="*30 + "\n")


def greeting_olms():
    print("\n" + "-="*40 + "\n" + "-"*80)
    print(f"{'Welcome to OLMS - Orbital LiDAR Management System!':^80}")
    print("-"*80 + "\n" + "-="*40 + "\n")

