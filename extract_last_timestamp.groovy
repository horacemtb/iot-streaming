import org.apache.commons.io.IOUtils
import java.nio.charset.StandardCharsets

// Get the FlowFile
flowFile = session.get()
if (flowFile != null) {
    // Read the content of the FlowFile
    def content = ''
    session.read(flowFile, { inputStream ->
        content = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
    } as InputStreamCallback)

    // Split the content into lines and extract the last line
    def lines = content.split('\n')
    def lastLine = lines[-1].trim()  // Get the last line and remove any leading/trailing whitespace

    // Extract the full timestamp from the last line (assuming it's the first two values)
    def timestampParts = lastLine.split(' ')
    def timestamp = timestampParts[0] + '_' + timestampParts[1]  // Combine the date and time with an underscore

    // Update the FlowFile attribute with the extracted timestamp
    flowFile = session.putAttribute(flowFile, 'extracted_timestamp', timestamp)

    // Transfer the FlowFile to the next processor
    session.transfer(flowFile, REL_SUCCESS)
}