import com.bazaarvoice.jolt.Chainr;

import com.bazaarvoice.jolt.SpecDriven;
import com.bazaarvoice.jolt.Transform;

public class TestCustomJoltTransform implements SpecDriven,Transform {

    final private Transform customTransform;

    public TestCustomJoltTransform(Object specJson) {
        this.customTransform = Chainr.fromSpec(specJson);
    }

    @Override
    public Object transform(Object o) {
        return customTransform.transform(o);
    }

    public static void main(String[] args) {
        System.out.println("This is a Test Custom Transform");
    }

}
