

def colors(txt, color=None, background=None, font=0):
    
    clr_prefix = '\033[' + str(font)
    if color is not None:
        clr_prefix += ';3' + str(color)
    if background is not None:
        clr_prefix += ';4' + str(background)
    clr_prefix += 'm'
    
    return clr_prefix + str(txt) + '\033[m'


def greeting_gediFinder():
    print('\n' + '-='*30 + '\n' + '-'*60)
    print(f'{"Welcome to GEDI Finder!":^60}')
    print('-'*60 + '\n' + '-='*30 + '\n')


def greeting_gedi2mongo():
    print('\n' + '-='*30 + '\n' + '-'*60)
    print(f'{"Welcome to GEDI 2 MongoDB!":^60}')
    print('-'*60 + '\n' + '-='*30 + '\n')

