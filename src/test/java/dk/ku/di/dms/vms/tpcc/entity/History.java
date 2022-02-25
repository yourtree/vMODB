package dk.ku.di.dms.vms.tpcc.entity;

import dk.ku.di.dms.vms.infra.AbstractEntityDefault;

import javax.persistence.*;

@Entity
@Table(name="history")
public class History extends AbstractEntityDefault {

//    @Id
//    @GeneratedValue(AUTO)
//    private Long id;

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
