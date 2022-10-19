package dk.ku.di.dms.vms.e_commerce.catalogue;

import dk.ku.di.dms.vms.modb.api.annotations.Query;
import dk.ku.di.dms.vms.modb.api.annotations.Repository;
import dk.ku.di.dms.vms.modb.api.interfaces.IRepository;

@Repository
public interface IProductRepository extends IRepository<Long, Product> {

    /*
    var baseQuery = "SELECT sock.sock_id AS id, sock.name, sock.description, sock.price, sock.count, sock.image_url_1, sock.image_url_2, GROUP_CONCAT(tag.name) AS tag_name
    FROM sock JOIN sock_tag ON sock.sock_id=sock_tag.sock_id JOIN tag ON sock_tag.tag_id=tag.tag_id"
     */

    @Query("WHERE tag.name IN (:tagsName)")
    Object baseQuery(String[] tagsName); // TODO make a DTO

}