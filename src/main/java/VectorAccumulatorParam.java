import org.apache.spark.AccumulatorParam;

import java.util.Vector;

/**
 * @author delia
 * @create 2016-09-23 下午4:42
 */

public class VectorAccumulatorParam implements AccumulatorParam<Vector> {

    @Override
    public Vector addAccumulator(Vector vector, Vector t1) {
        return null;
    }

    @Override
    public Vector addInPlace(Vector vector, Vector r1) {
        //vector.addInPlace(r1);
        return vector;
    }

    @Override
    public Vector zero(Vector initialValue) {
        //return Vector.zeros(initialValue.size());
        return null;
    }
}
