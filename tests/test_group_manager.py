from groupManager import GroupManager


def test_unregister_remove_so_o_peer_indicado():
    gm = GroupManager()
    a = gm.exposed_register("10.0.0.1", 6789, 5700)
    b = gm.exposed_register("10.0.0.2", 6790, 5701)

    gm.exposed_unregister(a)

    assert gm.exposed_list() == ((b, "10.0.0.2", 6790, 5701),)
