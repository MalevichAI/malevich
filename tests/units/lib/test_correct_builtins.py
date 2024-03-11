import os
import malevich
from malevich._utility.package import package_manager as pm 


def test_correct_builtins():
    packages = pm.get_all_packages()
    bultins = pm.builtins
    
    files = os.listdir(malevich.__path__[0])
    files = [file for file in files if os.path.isdir(os.path.join(malevich.__path__[0], file))]
    assert set(files) == set.union(set(packages), set(bultins))
    
    