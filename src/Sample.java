import java.util.ArrayList;

public class Sample
{
    private double cur_distance;
    private ArrayList<Double> features;
    private String categority;
    private int test_samle_index;
    public String getCategority() {
        return categority;
    }
    public void setCategority(String name)
    {
        this.categority = name;
    }
    public ArrayList<Double> getFeatures() {
        return features;
    }
    public void setFeatures(double x1,double x2,double x3,double x4)
    {
        features=new ArrayList<>();
        features.add(x1);
        features.add(x2);
        features.add(x3);
        features.add(x4);
    }
    public void set_test_Index(int index)
    {
        test_samle_index=index;
    }
    public int getTest_samle_index()
    {
        return test_samle_index;
    }

    public double getCur_distance()
    {
        return cur_distance;
    }
    public void setCur_distance(double distance)
    {
        cur_distance=distance;
    }
}