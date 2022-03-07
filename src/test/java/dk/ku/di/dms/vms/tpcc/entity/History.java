package dk.ku.di.dms.vms.tpcc.entity;

import dk.ku.di.dms.vms.annotations.VmsTable;
import dk.ku.di.dms.vms.infra.AbstractEntity;

import javax.persistence.*;

@Entity
@VmsTable(name="history")
public class History extends AbstractEntity<Long> {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    public Long id;

    @Column
    public Integer h_c_id;

    @Column
    public Integer h_c_d_id;

    @Column
    public Integer h_c_w_id;

    @Column
    public Integer h_d_id;

    @Column
    public Integer h_w_id;

    @Column
    public Long h_date;

    @Column
    public Float h_amount;

    @Column
    public String h_data;

    public History(){}

}
