import { Navigate } from "react-router-dom";

/** Bottom-nav + still opens Provider Studio (Cách C). */
export default function UploadPage() {
  return <Navigate to="/studio/creator_demo" replace />;
}
